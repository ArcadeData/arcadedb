# Bolt ResultSummary counters for write queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate Neo4j-driver `summary.counters()` for Bolt write queries by tracking CRUD/DDL counts in the Cypher engine and emitting a `stats` map in the Bolt terminal SUCCESS metadata.

**Architecture:** A single `QueryStatistics` accumulator lives on the shared `CommandContext` that every Cypher execution step already carries. Mutation steps increment it; the DDL path increments a local instance. `CypherExecutionPlan.execute()` and `OpenCypherQueryEngine.executeDDL()` attach it to the returned `ResultSet` through a new `ResultSet.getStatistics()` default method (mirroring `getExecutionPlan()`). `BoltNetworkExecutor` reads it at the terminal PULL/DISCARD and emits a Neo4j-style `stats` map.

**Tech Stack:** Java 21, native OpenCypher engine (`com.arcadedb.query.opencypher`), Bolt module (`com.arcadedb.bolt`), JUnit 5 + AssertJ, `neo4j-java-driver` for integration tests.

## Global Constraints

- Java 21+; import classes, do not use fully-qualified names.
- Use the `final` keyword on variables and parameters where possible.
- One-child `if` statements omit curly braces (match existing style).
- Performance/GC: read queries must allocate no `QueryStatistics` and take no new branches on the hot path; counters are primitive `int`.
- No new dependencies.
- All new server-side code covered by a test case; include a regression test.
- Do not add Claude as author anywhere; no issue numbers in Javadoc/source comments (behavioral invariants only).
- Cypher test language string is `"opencypher"` (both `command` and `query`).
- Remove any debug `System.out` before finishing.

---

### Task 1: `QueryStatistics` value object + `CommandContext` accessor

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/executor/QueryStatistics.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/CommandContext.java` (add one method to the interface)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/BasicCommandContext.java` (add a lazily-created field + accessor)
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/QueryStatisticsTest.java`

**Interfaces:**
- Produces: `class QueryStatistics` with primitive `int` fields and increment methods:
  `incNodesCreated()`, `incNodesDeleted()`, `incRelationshipsCreated()`, `incRelationshipsDeleted()`,
  `addPropertiesSet(int n)`, `addLabelsAdded(int n)`, `addLabelsRemoved(int n)`,
  `incIndexesAdded()`, `incIndexesRemoved()`, `incConstraintsAdded()`, `incConstraintsRemoved()`;
  getters `getNodesCreated()` ... `getConstraintsRemoved()`; `boolean containsUpdates()`.
- Produces: `QueryStatistics CommandContext.getStatistics()` - never null; lazily creates and caches on first call so mutation steps can increment; read queries never call it.

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryStatisticsTest {
  @Test
  void freshInstanceHasNoUpdates() {
    final QueryStatistics s = new QueryStatistics();
    assertThat(s.containsUpdates()).isFalse();
    assertThat(s.getNodesCreated()).isZero();
  }

  @Test
  void incrementsAreTracked() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.incNodesCreated();
    s.incRelationshipsCreated();
    s.addPropertiesSet(3);
    s.addLabelsAdded(2);
    assertThat(s.getNodesCreated()).isEqualTo(2);
    assertThat(s.getRelationshipsCreated()).isEqualTo(1);
    assertThat(s.getPropertiesSet()).isEqualTo(3);
    assertThat(s.getLabelsAdded()).isEqualTo(2);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void addingZeroPropertiesKeepsNoUpdatesWhenNothingElseChanged() {
    final QueryStatistics s = new QueryStatistics();
    s.addPropertiesSet(0);
    s.addLabelsAdded(0);
    assertThat(s.containsUpdates()).isFalse();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && mvn -q test -Dtest=QueryStatisticsTest`
Expected: FAIL - `QueryStatistics` does not exist (compilation error).

- [ ] **Step 3: Create `QueryStatistics`**

Copy the Apache license header from an existing engine file (e.g. `IteratorResultSet.java`) verbatim as the first lines, then:

```java
package com.arcadedb.query.sql.executor;

/**
 * Mutable, allocation-light accumulator of CRUD and schema mutation counts produced while executing
 * a single command. Held on the {@link CommandContext} and read once when the result set is
 * assembled. All counters are primitive ints; instances are created only for write commands.
 */
public class QueryStatistics {
  private int nodesCreated;
  private int nodesDeleted;
  private int relationshipsCreated;
  private int relationshipsDeleted;
  private int propertiesSet;
  private int labelsAdded;
  private int labelsRemoved;
  private int indexesAdded;
  private int indexesRemoved;
  private int constraintsAdded;
  private int constraintsRemoved;

  public void incNodesCreated()          { nodesCreated++; }
  public void incNodesDeleted()          { nodesDeleted++; }
  public void incRelationshipsCreated()  { relationshipsCreated++; }
  public void incRelationshipsDeleted()  { relationshipsDeleted++; }
  public void addPropertiesSet(final int n) { propertiesSet += n; }
  public void addLabelsAdded(final int n)   { labelsAdded += n; }
  public void addLabelsRemoved(final int n) { labelsRemoved += n; }
  public void incIndexesAdded()          { indexesAdded++; }
  public void incIndexesRemoved()        { indexesRemoved++; }
  public void incConstraintsAdded()      { constraintsAdded++; }
  public void incConstraintsRemoved()    { constraintsRemoved++; }

  public int getNodesCreated()          { return nodesCreated; }
  public int getNodesDeleted()          { return nodesDeleted; }
  public int getRelationshipsCreated()  { return relationshipsCreated; }
  public int getRelationshipsDeleted()  { return relationshipsDeleted; }
  public int getPropertiesSet()         { return propertiesSet; }
  public int getLabelsAdded()           { return labelsAdded; }
  public int getLabelsRemoved()         { return labelsRemoved; }
  public int getIndexesAdded()          { return indexesAdded; }
  public int getIndexesRemoved()        { return indexesRemoved; }
  public int getConstraintsAdded()      { return constraintsAdded; }
  public int getConstraintsRemoved()    { return constraintsRemoved; }

  public boolean containsUpdates() {
    return nodesCreated != 0 || nodesDeleted != 0 || relationshipsCreated != 0 || relationshipsDeleted != 0
        || propertiesSet != 0 || labelsAdded != 0 || labelsRemoved != 0
        || indexesAdded != 0 || indexesRemoved != 0 || constraintsAdded != 0 || constraintsRemoved != 0;
  }
}
```

- [ ] **Step 4: Add the `CommandContext.getStatistics()` interface method**

In `CommandContext.java`, next to `DatabaseInternal getDatabase();`, add:

```java
  QueryStatistics getStatistics();
```

- [ ] **Step 5: Implement in `BasicCommandContext`**

Add a field near `protected Map<String, Object> variables;`:

```java
  protected QueryStatistics statistics;
```

Add the accessor (lazy create so it is only allocated when a mutation step asks for it):

```java
  @Override
  public QueryStatistics getStatistics() {
    if (statistics == null)
      statistics = new QueryStatistics();
    return statistics;
  }
```

If any other `CommandContext` implementor exists that is not `BasicCommandContext` and does not inherit from it, add the same lazy accessor there. Run `grep -rl "implements CommandContext" engine/src/main` to confirm; `BasicCommandContext` is expected to be the only direct implementor.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd engine && mvn -q test -Dtest=QueryStatisticsTest`
Expected: PASS (3 tests).

- [ ] **Step 7: Compile the engine module**

Run: `cd engine && mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.

- [ ] **Step 8: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/QueryStatistics.java \
        engine/src/main/java/com/arcadedb/query/sql/executor/CommandContext.java \
        engine/src/main/java/com/arcadedb/query/sql/executor/BasicCommandContext.java \
        engine/src/test/java/com/arcadedb/query/sql/executor/QueryStatisticsTest.java
git commit -m "feat(#5000): QueryStatistics accumulator on CommandContext"
```

---

### Task 2: Surface statistics through `ResultSet`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/ResultSet.java` (add a default method near `getExecutionPlan()`, line ~64)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/IteratorResultSet.java` (carry an optional statistics reference)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/InternalResultSet.java` (carry an optional statistics reference)
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ResultSetStatisticsTest.java`

**Interfaces:**
- Consumes: `QueryStatistics` (Task 1).
- Produces: `default Optional<QueryStatistics> ResultSet.getStatistics()` returning `Optional.empty()`.
- Produces: `IteratorResultSet.setStatistics(QueryStatistics)` and `InternalResultSet.setStatistics(QueryStatistics)`, both returning `this`-friendly `void`, with overridden `getStatistics()`.

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSetStatisticsTest {
  @Test
  void defaultResultSetHasNoStatistics() {
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    assertThat(rs.getStatistics()).isEmpty();
  }

  @Test
  void attachedStatisticsAreReturned() {
    final QueryStatistics stats = new QueryStatistics();
    stats.incNodesCreated();
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    rs.setStatistics(stats);
    assertThat(rs.getStatistics()).isPresent();
    assertThat(rs.getStatistics().get().getNodesCreated()).isEqualTo(1);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && mvn -q test -Dtest=ResultSetStatisticsTest`
Expected: FAIL - `setStatistics` does not exist.

- [ ] **Step 3: Add the `ResultSet` default method**

In `ResultSet.java`, directly after the `getExecutionPlan()` default (line ~64-66), add:

```java
  default Optional<QueryStatistics> getStatistics() {
    return Optional.empty();
  }
```

(`java.util.*` is already imported, so `Optional` needs no new import.)

- [ ] **Step 4: Carry statistics in `IteratorResultSet`**

Add a field, setter, and override:

```java
  protected QueryStatistics statistics;

  public void setStatistics(final QueryStatistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public Optional<QueryStatistics> getStatistics() {
    return Optional.ofNullable(statistics);
  }
```

Add `import java.util.Optional;` if not present.

- [ ] **Step 5: Carry statistics in `InternalResultSet`**

Add the identical field, setter, and `getStatistics()` override in `InternalResultSet.java` (it already imports `Optional` for `getExecutionPlan`).

- [ ] **Step 6: Run test to verify it passes**

Run: `cd engine && mvn -q test -Dtest=ResultSetStatisticsTest`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/ResultSet.java \
        engine/src/main/java/com/arcadedb/query/sql/executor/IteratorResultSet.java \
        engine/src/main/java/com/arcadedb/query/sql/executor/InternalResultSet.java \
        engine/src/test/java/com/arcadedb/query/sql/executor/ResultSetStatisticsTest.java
git commit -m "feat(#5000): carry QueryStatistics on ResultSet"
```

---

### Task 3: Instrument CRUD mutation steps + surface from the plan

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CreateStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SetStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DeleteStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/RemoveStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/MergeStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherExecutionPlan.java` (attach stats in `execute()`)
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java`

**Interfaces:**
- Consumes: `context.getStatistics()` (Task 1), `IteratorResultSet.setStatistics` (Task 2).
- Produces: after `db.command("opencypher", <write>)`, `resultSet.getStatistics()` is present and reflects the writes.

**Counting semantics (Neo4j-compatible, pinned by the test):**
- `CREATE (n:A:B {p:1,q:2})` -> nodesCreated +1, labelsAdded +2, propertiesSet +2. A label-less `CREATE ()` adds 0 labels.
- `CREATE ()-[:R {w:1}]->()` -> relationshipsCreated +1, propertiesSet +1 (edge property).
- `SET n.x = 1` -> propertiesSet +1; `SET n:Label` -> labelsAdded +1.
- `REMOVE n.x` -> propertiesSet +1 (Neo4j counts removals under propertiesSet); `REMOVE n:Label` -> labelsRemoved +1.
- `DELETE n` (vertex) -> nodesDeleted +1; `DELETE r` (edge) -> relationshipsDeleted +1.
- `MERGE` that creates routes through the create/set counting; a `MERGE` that matches an existing node adds nothing.

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that write commands populate QueryStatistics with Neo4j-compatible CRUD counts,
 * and that read queries carry no statistics.
 */
class CypherQueryStatisticsTest extends CypherTestBase {

  private QueryStatistics statsOf(final Database db, final String cypher) {
    final ResultSet rs = db.command("opencypher", cypher);
    while (rs.hasNext())
      rs.next();
    final Optional<QueryStatistics> s = rs.getStatistics();
    assertThat(s).isPresent();
    return s.get();
  }

  @Test
  void createNodesRelationshipAndProperties() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE (:Beer {name:'IPA'})-[:BREWED_BY {since:1990}]->(:Brewery {name:'Acme'})");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getRelationshipsCreated()).isEqualTo(1);
      assertThat(s.getPropertiesSet()).isEqualTo(3); // name, since, name
      assertThat(s.getLabelsAdded()).isEqualTo(2);   // Beer, Brewery
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void setPropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'a'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Person {name:'a'}) SET n.age = 30, n:Employee");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsAdded()).isEqualTo(1);
    });
  }

  @Test
  void removePropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag:Old {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Tag {name:'x'}) REMOVE n.name, n:Old");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsRemoved()).isEqualTo(1);
    });
  }

  @Test
  void deleteNode() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Doomed {id:1})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Doomed {id:1}) DELETE n");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
    });
  }

  @Test
  void mergeCreatesOnlyOnce() {
    database.transaction(() -> {
      final QueryStatistics first = statsOf(database, "MERGE (:City {name:'Rome'})");
      assertThat(first.getNodesCreated()).isEqualTo(1);
      final ResultSet rs = database.command("opencypher", "MERGE (:City {name:'Rome'})");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics().get().getNodesCreated()).isZero();
    });
  }

  @Test
  void readQueryHasNoStatistics() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:R {n:1})");
      final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics()).isEmpty();
    });
  }
}
```

Note: match the base-class harness used by existing Cypher tests. Inspect a sibling test (e.g. `engine/src/test/java/com/arcadedb/query/opencypher/` - find one extending a shared base that exposes a `database` field). If the base class is named differently than `CypherTestBase`, use the actual name and its setup idiom; keep the test bodies unchanged.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && mvn -q test -Dtest=CypherQueryStatisticsTest`
Expected: FAIL - `getStatistics()` empty for writes (counters not yet incremented / not surfaced).

- [ ] **Step 3: Instrument `CreateStep`**

In `createVertex(...)`, after `vertex.save();` (line ~386) and OUTSIDE the `if (context.isProfiling())` block, add unconditional counting:

```java
    vertex.save();
    final QueryStatistics stats = context.getStatistics();
    stats.incNodesCreated();
    if (nodePattern.hasLabels())
      stats.addLabelsAdded(nodePattern.getLabels().size());
    stats.addPropertiesSet(vertex.getPropertyNames().size());
```

Keep the existing profiling block (`vertexCount++` etc.) intact below it. In `createEdge(...)`, after the edge is created (line ~427), add:

```java
    final QueryStatistics stats = context.getStatistics();
    stats.incRelationshipsCreated();
    stats.addPropertiesSet(edge.getPropertyNames().size());
```

Add `import com.arcadedb.query.sql.executor.QueryStatistics;`. `MutableVertex`/`MutableEdge` expose `getPropertyNames()` (a `Set<String>`); for a freshly created record it equals the user properties just set.

- [ ] **Step 4: Instrument `SetStep`**

At each property write site (`mutableDoc.set(name, value)` followed by `save()`), increment once per property assigned. Where a whole map is applied, increment by the number of entries. Add near the top of the row-processing method:

```java
    final QueryStatistics stats = context.getStatistics();
```

and after each `set(...)`:

```java
    stats.addPropertiesSet(1);
```

For `SET n:Label` handling, add `stats.addLabelsAdded(<number of labels added>)`. Follow the existing label-set branch in `SetStep`. Add the `QueryStatistics` import.

- [ ] **Step 5: Instrument `RemoveStep`**

In `removeProperty(...)` after `mutableDoc.remove(property)` / `save()`, add `context.getStatistics().addPropertiesSet(1);`. In `removeLabels(...)`, add `context.getStatistics().addLabelsRemoved(<number of labels removed>);`. Add the import.

- [ ] **Step 6: Instrument `DeleteStep`**

In `deleteVertex(...)` before/after `vertex.delete()`, add `context.getStatistics().incNodesDeleted();`. In `deleteEdge(...)` after `edge.delete()`, add `context.getStatistics().incRelationshipsDeleted();`. For the generic `.delete()` sites at other lines, determine whether the record is a vertex or edge (`record instanceof Vertex`) and increment the matching counter. Add the import.

- [ ] **Step 7: Verify `MergeStep` routes through counting**

`MergeStep` performs its create via the same create/set logic. Confirm its create branch either delegates to the shared create helper or performs `newVertex/newEdge` + `save` directly. If it creates directly (not via `CreateStep`), add the same increments used in Step 3 at its create sites. If it delegates, no change is needed. Match branch (existing node found) must add nothing. Add the import if edits are needed.

- [ ] **Step 8: Surface statistics from `CypherExecutionPlan.execute()`**

In `execute()`, inside the `if (hasWriteOps)` block (lines ~267-279), read the accumulator once and attach it to both write return paths:

```java
    if (hasWriteOps) {
      final List<ResultInternal> materializedResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        materializedResults.add((ResultInternal) resultSet.next());
      }
      final QueryStatistics stats = context.getStatistics();
      if (statement.getReturnClause() == null || statement.hasFinishClause()) {
        final IteratorResultSet empty = new IteratorResultSet(Collections.<Result>emptyList().iterator());
        empty.setStatistics(stats);
        return empty;
      }
      final IteratorResultSet out = new IteratorResultSet(materializedResults.iterator());
      out.setStatistics(stats);
      return out;
    }
```

Use `context.getStatistics()` (never null after a write, since the mutation steps already created it; if a "write" statement performed no mutation it returns a fresh empty-but-present accumulator, which is correct: `containsUpdates()` is false). Add `import com.arcadedb.query.sql.executor.QueryStatistics;`.

- [ ] **Step 9: Run the test to verify it passes**

Run: `cd engine && mvn -q test -Dtest=CypherQueryStatisticsTest`
Expected: PASS (6 tests). If `propertiesSet` for CREATE differs (e.g. system properties counted), adjust the counting site to count only user-supplied property keys and re-run - the test values are the contract.

- [ ] **Step 10: Run the broader Cypher mutation regression subset**

Run: `cd engine && mvn -q test -Dtest="Cypher*Create*,Cypher*Merge*,Cypher*Delete*,Cypher*Set*"`
Expected: PASS - no regressions in existing mutation tests.

- [ ] **Step 11: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CreateStep.java \
        engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SetStep.java \
        engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DeleteStep.java \
        engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/RemoveStep.java \
        engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/MergeStep.java \
        engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherExecutionPlan.java \
        engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java
git commit -m "feat(#5000): track CRUD counts in Cypher mutation steps"
```

---

### Task 4: Instrument the Cypher DDL path (indexes/constraints)

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java` (`executeDDL` + the four `executeCreate/Drop*` methods)
- Test: add cases to `engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java`

**Interfaces:**
- Consumes: `QueryStatistics` (Task 1), `InternalResultSet.setStatistics` (Task 2).
- Produces: `db.command("opencypher", "CREATE INDEX ...")` returns a ResultSet whose `getStatistics().get().getIndexesAdded() == 1`.

**Counting semantics:** CREATE INDEX -> indexesAdded +1 (0 if `IF NOT EXISTS` and it already existed); DROP INDEX -> indexesRemoved +1 (0 if `IF EXISTS` and absent). CREATE/DROP CONSTRAINT -> constraintsAdded/Removed +1 with the same no-op guards.

- [ ] **Step 1: Add failing DDL test cases**

Append to `CypherQueryStatisticsTest`:

```java
  @Test
  void createIndexCounts() {
    database.command("opencypher", "CREATE (:Product {sku:'a'})"); // ensure type exists
    final ResultSet rs = database.command("opencypher", "CREATE INDEX FOR (p:Product) ON (p.sku)");
    while (rs.hasNext()) rs.next();
    assertThat(rs.getStatistics()).isPresent();
    assertThat(rs.getStatistics().get().getIndexesAdded()).isEqualTo(1);
    assertThat(rs.getStatistics().get().containsUpdates()).isTrue();
  }
```

(Use the exact CREATE INDEX / CREATE CONSTRAINT syntax the parser accepts - confirm against an existing DDL test under `engine/src/test/java/com/arcadedb/query/opencypher/` before finalizing the query strings.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && mvn -q test -Dtest=CypherQueryStatisticsTest#createIndexCounts`
Expected: FAIL - statistics empty (DDL returns a bare `InternalResultSet`).

- [ ] **Step 3: Thread a `QueryStatistics` through `executeDDL`**

Rewrite `executeDDL` to accumulate and attach:

```java
  private ResultSet executeDDL(final CypherDDLStatement ddl) {
    final Schema schema = database.getSchema();
    final QueryStatistics stats = new QueryStatistics();

    switch (ddl.getKind()) {
    case CREATE_CONSTRAINT:
      executeCreateConstraint(ddl, schema, stats);
      break;
    case DROP_CONSTRAINT:
      executeDropConstraint(ddl, schema, stats);
      break;
    case CREATE_INDEX:
      executeCreateIndex(ddl, schema, stats);
      break;
    case DROP_INDEX:
      executeDropIndex(ddl, schema, stats);
      break;
    }

    final InternalResultSet result = new InternalResultSet();
    result.setStatistics(stats);
    return result;
  }
```

Add `stats` as a parameter to the four `executeCreate/Drop*` methods and increment inside them at the point the schema change actually succeeds (after the builder `create()` / index build call, and only on the branch that performed the change - not the `ignoreIfExists`/`ifExists` no-op branch). Example for `executeCreateIndex`: after the index is created, `stats.incIndexesAdded();`. Add `import com.arcadedb.query.sql.executor.QueryStatistics;`.

- [ ] **Step 4: Run DDL test to verify it passes**

Run: `cd engine && mvn -q test -Dtest=CypherQueryStatisticsTest`
Expected: PASS (7 tests).

- [ ] **Step 5: Run the Cypher DDL regression subset**

Run: `cd engine && mvn -q test -Dtest="*Cypher*Index*,*Cypher*Constraint*,*Cypher*DDL*"`
Expected: PASS - no regressions.

- [ ] **Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java \
        engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java
git commit -m "feat(#5000): count Cypher index/constraint DDL"
```

---

### Task 5: Emit the Bolt `stats` map

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (`handlePull` terminal block ~700-731, `handleDiscard` ~777-786)
- Create: `bolt/src/main/java/com/arcadedb/bolt/BoltResultStats.java` (maps `QueryStatistics` -> Neo4j `stats` map)
- Test: `bolt/src/test/java/com/arcadedb/bolt/Bolt5000ResultCountersIT.java`

**Interfaces:**
- Consumes: `currentResultSet.getStatistics()` (Task 2).
- Produces: terminal PULL/DISCARD SUCCESS metadata contains `stats` -> `Map<String,Object>` with hyphenated Neo4j keys + `contains-updates=true`, only when the result carries a statistics object with updates.

**Neo4j `stats` map keys:** `nodes-created`, `nodes-deleted`, `relationships-created`, `relationships-deleted`, `properties-set`, `labels-added`, `labels-removed`, `indexes-added`, `indexes-removed`, `constraints-added`, `constraints-removed`, `contains-updates`. Emit only non-zero counters, always include `contains-updates`.

- [ ] **Step 1: Write the failing Bolt IT**

```java
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.summary.SummaryCounters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wire test: a Bolt write query must populate summary.counters() on the neo4j driver.
 */
public class Bolt5000ResultCountersIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void writeCountersReflectActualWrites() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final SummaryCounters counters = session.run(
          "CREATE (:Beer {name:$n})-[:BREWED_BY]->(:Brewery {name:$b})",
          org.neo4j.driver.Values.parameters("n", "IPA", "b", "Acme")).consume().counters();
      assertThat(counters.nodesCreated()).isEqualTo(2);
      assertThat(counters.relationshipsCreated()).isEqualTo(1);
      assertThat(counters.propertiesSet()).isEqualTo(2);
      assertThat(counters.containsUpdates()).isTrue();
    }
  }

  @Test
  void readQueryReportsNoUpdates() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:Thing {id:1})").consume();
      final SummaryCounters counters = session.run("MATCH (n:Thing) RETURN n").consume().counters();
      assertThat(counters.containsUpdates()).isFalse();
      assertThat(counters.nodesCreated()).isZero();
    }
  }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd bolt && mvn -q -DskipITs=false test -Dtest=Bolt5000ResultCountersIT`
Expected: FAIL - `nodesCreated()` is 0 (no `stats` emitted).

- [ ] **Step 3: Create the Bolt stats mapper**

Copy the Apache header, then:

```java
package com.arcadedb.bolt;

import com.arcadedb.query.sql.executor.QueryStatistics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps engine QueryStatistics to the Neo4j Bolt SUCCESS "stats" metadata map. Emits only non-zero
 * counters plus contains-updates, matching how Neo4j server reports write summaries.
 */
final class BoltResultStats {
  private BoltResultStats() {
  }

  static Map<String, Object> toStatsMap(final QueryStatistics s) {
    final Map<String, Object> stats = new LinkedHashMap<>();
    putIfNonZero(stats, "nodes-created", s.getNodesCreated());
    putIfNonZero(stats, "nodes-deleted", s.getNodesDeleted());
    putIfNonZero(stats, "relationships-created", s.getRelationshipsCreated());
    putIfNonZero(stats, "relationships-deleted", s.getRelationshipsDeleted());
    putIfNonZero(stats, "properties-set", s.getPropertiesSet());
    putIfNonZero(stats, "labels-added", s.getLabelsAdded());
    putIfNonZero(stats, "labels-removed", s.getLabelsRemoved());
    putIfNonZero(stats, "indexes-added", s.getIndexesAdded());
    putIfNonZero(stats, "indexes-removed", s.getIndexesRemoved());
    putIfNonZero(stats, "constraints-added", s.getConstraintsAdded());
    putIfNonZero(stats, "constraints-removed", s.getConstraintsRemoved());
    stats.put("contains-updates", true);
    return stats;
  }

  private static void putIfNonZero(final Map<String, Object> stats, final String key, final int value) {
    if (value != 0)
      stats.put(key, (long) value);
  }
}
```

(Bolt integers are encoded as longs; cast keeps the driver's counter parsing happy.)

- [ ] **Step 4: Emit `stats` in `handlePull`**

In the terminal `if (!hasMore)` block, BEFORE `currentResultSet.close()` (line ~716), read and attach:

```java
        if (currentResultSet != null) {
          final Optional<QueryStatistics> stats = currentResultSet.getStatistics();
          if (stats.isPresent() && stats.get().containsUpdates())
            metadata.put("stats", BoltResultStats.toStatsMap(stats.get()));
        }
```

Place it just inside the `if (!hasMore)` block, above the existing `if (currentResultSet != null) { close... }`. Add `import com.arcadedb.query.sql.executor.QueryStatistics;` and `import java.util.Optional;` if absent.

- [ ] **Step 5: Emit `stats` in `handleDiscard`**

Before the drain/close at line ~761, capture the statistics (they are computed at execute time, so reading before drain is fine):

```java
    if (currentResultSet != null) {
      final Optional<QueryStatistics> stats = currentResultSet.getStatistics();
      if (stats.isPresent() && stats.get().containsUpdates())
        metadata.put("stats", BoltResultStats.toStatsMap(stats.get()));
      ...existing drain + close...
    }
```

Note `metadata` is declared at line ~777; move the statistics capture to after `metadata` is created, or hoist a local `QueryStatistics` captured before the drain and add it to `metadata` after creation. Keep the `stats` entry out of read-query responses (guarded by `containsUpdates()`).

- [ ] **Step 6: Run the Bolt IT to verify it passes**

Run: `cd bolt && mvn -q -DskipITs=false test -Dtest=Bolt5000ResultCountersIT`
Expected: PASS (2 tests).

- [ ] **Step 7: Run the existing Bolt IT subset for regressions**

Run: `cd bolt && mvn -q -DskipITs=false test -Dtest="Bolt*"`
Expected: PASS - existing Bolt suites unaffected.

- [ ] **Step 8: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/BoltResultStats.java \
        bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/test/java/com/arcadedb/bolt/Bolt5000ResultCountersIT.java
git commit -m "feat(#5000): emit Bolt stats map for write summaries"
```

---

### Task 6: Flip RESULT-004 to passing in the conformance spec

**Files:**
- Modify: `bolt/conformance/spec.yaml` (RESULT-004, lines ~316-339)

**Interfaces:**
- Consumes: nothing (documentation/state flip).
- Produces: `RESULT-004.current_status: passing`; removes the `known_limitation`/`tracking_issue` markers per the spec-validator's rules for passing cells.

- [ ] **Step 1: Inspect the spec-validator's requirements**

Run: `grep -rn "current_status\|known_limitation\|tracking_issue\|passing\|expected-fail" bolt/conformance/ | grep -i "valid\|require\|test" | head`
Also read the validator test (search `grep -rl "spec.yaml\|current_status" --include=*.java bolt engine server`) to learn whether a `passing` cell must NOT have `tracking_issue`/`known_limitation`. Follow whatever the validator enforces (mirror how a currently-`passing` cell in `spec.yaml` is shaped).

- [ ] **Step 2: Edit RESULT-004**

Set `current_status: passing`. If the validator forbids `known_limitation`/`tracking_issue` on passing cells (confirmed in Step 1), remove those two keys from RESULT-004. Match the exact shape of an existing `passing` scenario in the same file.

- [ ] **Step 3: Run the spec-validator test**

Run: `grep -rl "spec.yaml" --include=*.java . | head` then run that test class, e.g. `cd bolt && mvn -q test -Dtest=<SpecValidatorTestClass>`
Expected: PASS - spec remains valid with RESULT-004 flipped.

- [ ] **Step 4: Commit**

```bash
git add bolt/conformance/spec.yaml
git commit -m "test(#5000): flip RESULT-004 to passing"
```

---

### Task 7: Full-module verification + open the HTTP/gRPC follow-up issue

**Files:** none (verification + issue creation).

- [ ] **Step 1: Build and test the touched modules**

Run: `mvn -q -pl engine,bolt -am install`
Expected: BUILD SUCCESS with all new and existing tests green.

- [ ] **Step 2: Confirm no debug output or stray fully-qualified names remain**

Run: `git diff origin/main --unified=0 | grep -nE "System\.out|System\.err"`
Expected: no matches.

- [ ] **Step 3: Open the follow-up issue**

Open a GitHub issue (child of #4890, milestone 26.7.2): "HTTP/gRPC: surface QueryStatistics write counters via ResultSet.getStatistics()". Motivation: the engine now tracks CRUD/DDL counts and exposes them via `ResultSet.getStatistics()`; HTTP `/command` and gRPC hold a `ResultSet` and can surface the same counters cheaply. Acceptance: HTTP `/command` JSON includes a `stats` object; gRPC result summary carries the counters; both covered by tests. Reference this PR.

- [ ] **Step 4: Final commit (if any doc/notes changed)**

Only if files changed in this task; otherwise skip.

---

## Self-Review

**Spec coverage:**
- Two missing layers (Bolt `stats` + engine counters) -> Tasks 3-5. ✓
- `QueryStatistics` allocation-light, primitive ints, off read hot path -> Task 1 (lazy accessor) + Task 3 read-query test asserting empty statistics. ✓
- Full Neo4j counter set incl. DDL -> Task 3 (CRUD) + Task 4 (indexes/constraints). ✓
- Reusable channel via `ResultSet.getStatistics()` default -> Task 2. ✓
- Bolt terminal PULL + DISCARD -> Task 5 (both handlers). ✓
- RESULT-004 flip -> Task 6. ✓
- HTTP/gRPC deferred to follow-up issue -> Task 7. ✓
- No stats on read queries (matches Neo4j) -> Task 3 read test + Task 5 `containsUpdates()` guard + Task 5 read IT. ✓

**Placeholder scan:** No TBD/TODO. Instrumentation steps that cannot be pinned to an exact current line (SetStep/RemoveStep/DeleteStep/MergeStep internal sites, DDL no-op guards, spec-validator rules) carry explicit "confirm against sibling X" instructions plus the exact increment call to add - the ambiguity is in *where the existing code is*, not in *what to write*.

**Type consistency:** `QueryStatistics` method names (`incNodesCreated`, `addPropertiesSet(int)`, `addLabelsAdded(int)`, `getNodesCreated()`, `containsUpdates()`) are identical across Tasks 1, 3, 4, 5. `ResultSet.getStatistics()` returns `Optional<QueryStatistics>` in Tasks 2 and 5. `IteratorResultSet.setStatistics` / `InternalResultSet.setStatistics` used consistently in Tasks 2, 3, 4. Bolt map keys match the design's Neo4j key list.
