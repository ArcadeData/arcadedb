/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FOREACH: CASE and EXISTS in the body, body scope, created counts and visibility in RETURN.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherForeachTest {
  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/4993
   * <p>
   * FOREACH with CASE WHEN exists(pattern) must honor the CASE condition: the FOREACH body
   * must only execute for rows where the pattern predicate is true.
   */
  @Nested
  class Issue4993ForeachCaseExists {
    private Database database;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      final String databasePath = "./target/databases/testopencypher-4993-" + testInfo.getTestMethod().get().getName();
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private void seed() {
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (a:User {active: true}), (b:User {active: false}), (a)-[:FRIEND]->(b)");
        database.command("opencypher", "CREATE (c:User {active: true})");
      });
    }

    private long countFlagged() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      return ((Number) r.getProperty("cnt")).longValue();
    }

    @Test
    void foreachCaseWhenExistsFunctionStyle() {
      seed();
      database.transaction(() ->
          database.command("opencypher",
              """
              MATCH (u:User {active: true})
              FOREACH (ignore IN CASE WHEN EXISTS((u)-[:FRIEND]->(:User {active: false})) THEN [1] ELSE [] END |
                SET u.flagged = true
              )"""));

      assertThat(countFlagged()).isEqualTo(1L);
    }

    @Test
    void foreachCaseWhenSizeControlCase() {
      seed();
      database.transaction(() ->
          database.command("opencypher",
              """
              MATCH (u:User {active: true})
              FOREACH (ignore IN CASE WHEN size([(u)-[:FRIEND]->(f:User) WHERE f.active = false | f]) > 0 THEN [1] ELSE [] END |
                SET u.flagged = true
              )"""));

      assertThat(countFlagged()).isEqualTo(1L);
    }

    @Test
    void existsFunctionStyleInReturn() {
      // The same function-style exists(pattern) must evaluate as a pattern predicate in any expression
      // context, not just FOREACH. Here it is projected directly in RETURN.
      seed();
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (u:User {active: true})
          RETURN u.active AS active, EXISTS((u)-[:FRIEND]->(:User {active: false})) AS hasInactiveFriend
          ORDER BY hasInactiveFriend DESC""");

      assertThat(rs.hasNext()).isTrue();
      assertThat((Boolean) rs.next().getProperty("hasInactiveFriend")).isTrue();
      assertThat(rs.hasNext()).isTrue();
      assertThat((Boolean) rs.next().getProperty("hasInactiveFriend")).isFalse();
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void foreachCaseWhenExistsSubqueryControlCase() {
      seed();
      database.transaction(() ->
          database.command("opencypher",
              """
              MATCH (u:User {active: true})
              FOREACH (ignore IN CASE WHEN EXISTS { (u)-[:FRIEND]->(:User {active: false}) } THEN [1] ELSE [] END |
                SET u.flagged = true
              )"""));

      assertThat(countFlagged()).isEqualTo(1L);
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/5114
   * <p>
   * FOREACH mutations to a matched node must be visible to a RETURN in the same query,
   * matching Neo4j / Memgraph / FalkorDB behaviour (and ArcadeDB's own direct SET behaviour).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5114ForeachReturnVisibility {
    private Database database;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      final String databasePath = "./target/databases/testopencypher-5114-" + testInfo.getTestMethod().get().getName();
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private long commandVal(final String query) {
      final ResultSet rs = database.command("opencypher", query);
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      return ((Number) r.getProperty("val")).longValue();
    }

    private long queryVal(final String query) {
      final ResultSet rs = database.query("opencypher", query);
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      return ((Number) r.getProperty("val")).longValue();
    }

    @Test
    void foreachSetVisibleToReturn() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 0, val: 0})"));

      database.transaction(() -> {
        final long sameQuery = commandVal("""
            MATCH (n:Node {id: 0})
            FOREACH (x IN [1] | SET n.val = 42)
            RETURN n.val AS val""");
        assertThat(sameQuery).isEqualTo(42L);
      });

      // Subsequent read must still be 42 (mutation persisted).
      assertThat(queryVal("MATCH (n:Node {id: 0}) RETURN n.val AS val")).isEqualTo(42L);
    }

    @Test
    void multipleForeachBlocksVisibleToReturn() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 2, val: 0})"));

      database.transaction(() -> {
        final long sameQuery = commandVal("""
            MATCH (n:Node {id: 2})
            FOREACH (x IN [1] | SET n.val = 10)
            FOREACH (y IN [1] | SET n.val = 20)
            FOREACH (z IN [1] | SET n.val = 30)
            RETURN n.val AS val""");
        assertThat(sameQuery).isEqualTo(30L);
      });

      assertThat(queryVal("MATCH (n:Node {id: 2}) RETURN n.val AS val")).isEqualTo(30L);
    }

    @Test
    void foreachIncrementVisibleToReturn() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 3, val: 0})"));

      database.transaction(() -> {
        final long sameQuery = commandVal("""
            MATCH (n:Node {id: 3})
            FOREACH (x IN [1, 2, 3] | SET n.val = n.val + 1)
            RETURN n.val AS val""");
        assertThat(sameQuery).isEqualTo(3L);
      });

      assertThat(queryVal("MATCH (n:Node {id: 3}) RETURN n.val AS val")).isEqualTo(3L);
    }

    @Test
    void directSetStillVisibleToReturn() {
      // Control: direct SET already works; guard against regressions.
      database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 1, val: 0})"));

      database.transaction(() -> {
        final long sameQuery = commandVal("""
            MATCH (n:Node {id: 1})
            SET n.val = 42
            RETURN n.val AS val""");
        assertThat(sameQuery).isEqualTo(42L);
      });
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/5166
   * <p>
   * When {@code FOREACH ... CREATE} is directly followed by {@code RETURN count(var)} (an
   * aggregation over the outer MATCH variable), the {@code TypeCountStep} O(1) optimization
   * short-circuited the whole plan, silently discarding the FOREACH write side effects.
   * Neo4j persists the created node; ArcadeDB must too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5166ForeachCreateCount {
    private Database database;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      final String databasePath = "./target/databases/testopencypher-5166-" + testInfo.getTestMethod().get().getName();
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private long count(final String label) {
      final ResultSet rs = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c");
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }

    @Test
    void foreachCreateFollowedByCountVarPersists() {
      database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", """
            MATCH (a:A)
            FOREACH (x IN [1] | CREATE (:FB {id: x}))
            RETURN count(a) AS c""");
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("c")).longValue()).isEqualTo(1L);
      });

      // The FB node created inside FOREACH must be persisted (was silently lost before the fix).
      assertThat(count("FB")).isEqualTo(1L);
    }

    @Test
    void foreachCreateMultipleFollowedByCountVarPersists() {
      database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", """
            MATCH (a:A)
            FOREACH (x IN [1, 2, 3] | CREATE (:FB {id: x}))
            RETURN count(a) AS c""");
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      });

      assertThat(count("FB")).isEqualTo(3L);
    }

    @Test
    void foreachCreateWithIntermediateWithStillWorks() {
      // The documented workaround must keep working.
      database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", """
            MATCH (a:A)
            FOREACH (x IN [1] | CREATE (:FB {id: x}))
            WITH a
            RETURN count(a) AS c""");
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      });

      assertThat(count("FB")).isEqualTo(1L);
    }

    @Test
    void plainMatchCountVarStillOptimized() {
      // Control: the O(1) TypeCountStep optimization must still apply when there is no FOREACH.
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:A {id: 1})");
        database.command("opencypher", "CREATE (:A {id: 2})");
        database.command("opencypher", "CREATE (:A {id: 3})");
      });

      assertThat(count("A")).isEqualTo(3L);
    }
  }

  /**
   * Regression tests for issue #8105: {@code CypherSemanticValidator.validateVariableScope} skipped the
   * body of a {@code FOREACH} clause entirely, so a body writing through a variable a preceding
   * {@code WITH} had dropped - or one that was never bound at all - was accepted, did nothing, and
   * reported success instead of raising {@code UndefinedVariable} the way Neo4j does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8105ForeachBodyScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:Function {Name:'Zeta'})");
    }

    /** The reporter's query: `f` is dropped by the projecting WITH, then written to inside FOREACH's SET. */
    @Test
    void foreachSetReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (f:Function) \
          WITH f.Name AS BF \
          FOREACH (x IN [1] | SET f.Name = 'q') \
          RETURN BF""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /** Same shape, but the never-bound name is referenced inside FOREACH's REMOVE. */
    @Test
    void foreachRemoveReferencingUnboundVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (f:Function) \
          FOREACH (x IN [1] | REMOVE ghost.Name) \
          RETURN f.Name AS BusinessFunction""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'ghost'");
    }

    /** Same shape, but the out-of-scope reference is inside a FOREACH nested two levels deep. */
    @Test
    void nestedForeachReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (f:Function) \
          WITH f.Name AS BF \
          FOREACH (x IN [1] | FOREACH (y IN [1] | SET f.Name = 'q')) \
          RETURN BF""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /** Control: a variable the FOREACH body can legitimately see still works, and the write takes effect. */
    @Test
    void foreachSetReferencingInScopeVariableSucceeds() {
      database.command("opencypher",
          """
          MATCH (f:Function) \
          FOREACH (x IN [1] | SET f.Name = 'Updated')""").close();

      try (final ResultSet rs = database.query("opencypher", "MATCH (f:Function) RETURN f.Name AS name")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Updated");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Control: the FOREACH's own loop variable is in scope inside its body. */
    @Test
    void foreachLoopVariableIsInScopeInsideBody() {
      database.command("opencypher",
          "CREATE (:Counter {Value:0})").close();
      database.command("opencypher",
          """
          MATCH (c:Counter) \
          FOREACH (x IN [1,2,3] | SET c.Value = x)""").close();

      try (final ResultSet rs = database.query("opencypher", "MATCH (c:Counter) RETURN c.Value AS v")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(3);
      }
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/11684
   * <p>
   * A write inside FOREACH must be observable, later in the same statement, through every alias of the node it wrote:
   * #5114 covered the node variable and #10890 the path, this covers a node held in a {@code collect()} list and read
   * back through {@code UNWIND}, and an outer variable whose node the body wrote through a MERGE-bound alias.
   */
  @Nested
  class Issue11684ForeachAliasRefresh extends TestHelper {
    @Override
    protected void beginTest() {
      database.transaction(() -> database.command("opencypher", """
          CREATE (a:Node {id:1,v:1,s:'a',b:true,l:[1,2]})
          CREATE (b:Node {id:2,v:2,s:'b',b:false,l:[]})
          CREATE (c:Node {id:3,v:3,s:'c',b:true,l:[3]})
          CREATE (a)-[:R {k:1}]->(b)
          CREATE (b)-[:R {k:2}]->(c)""").close());
    }

    /** The reporter's first query: the nodes written through the FOREACH loop variable are read back through UNWIND. */
    @Test
    void listBoundNodesObserveForeachWrite() {
      database.transaction(() -> assertThat(idToV("""
          MATCH (a:Node) WHERE a.id <= 2
          WITH collect(a) AS ns
          FOREACH (n IN ns | SET n.v = n.v + 10)
          WITH ns UNWIND ns AS m
          RETURN m.id AS id, m.v AS v ORDER BY id""")).containsExactly(1L, 11L, 2L, 12L));

      assertThat(committedV()).containsExactly(1L, 11L, 2L, 12L, 3L, 3L);
    }

    /** The reporter's second query: the body writes the node through the MERGE-bound m, the outer n is read after it. */
    @Test
    void outerVariableObservesWriteThroughMergeAlias() {
      database.transaction(() -> assertThat(single("""
          MATCH (n:Node {id:3})
          FOREACH (x IN [1] | MERGE (m:Node {id:3}) SET m.v = 99)
          RETURN n.v AS nv""", "nv")).isEqualTo(99L));

      assertThat(committedV()).containsExactly(1L, 1L, 2L, 2L, 3L, 99L);
    }

    /** Each iteration reads the outer n, so the write the previous iteration made through m must already show through it. */
    @Test
    void mergeAliasWriteAccumulatesAcrossIterations() {
      database.transaction(() -> assertThat(single("""
          MATCH (n:Node {id:3})
          FOREACH (x IN [1, 2, 3] | MERGE (m:Node {id:3}) SET m.v = n.v + 1)
          RETURN n.v AS nv""", "nv")).isEqualTo(6L));

      assertThat(committedV()).containsExactly(1L, 1L, 2L, 2L, 3L, 6L);
    }

    /** A list carrying the same node twice: both writes land on it, and every element of the list reads the result. */
    @Test
    void listWithDuplicateNodeObservesEveryWrite() {
      database.transaction(() -> assertThat(idToV("""
          MATCH (a:Node {id:1})
          WITH [a, a] AS ns
          FOREACH (n IN ns | SET n.v = n.v + 10)
          UNWIND ns AS m
          RETURN m.id AS id, m.v AS v""")).containsExactly(1L, 21L, 1L, 21L));

      assertThat(committedV()).containsExactly(1L, 21L, 2L, 2L, 3L, 3L);
    }

    /** A node nested in a list of lists and in a map is the same node, so it reads the write too. */
    @Test
    void nestedListAndMapObserveForeachWrite() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", """
            MATCH (a:Node {id:1})
            WITH collect(a) AS ns, [[a]] AS nested, {node: a} AS holder
            FOREACH (n IN ns | SET n.v = 42)
            RETURN nested[0][0].v AS nestedV, holder.node.v AS mapV""")) {
          final Result row = rs.next();
          assertThat(row.<Number>getProperty("nestedV").longValue()).isEqualTo(42L);
          assertThat(row.<Number>getProperty("mapV").longValue()).isEqualTo(42L);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /** The write is made by a nested FOREACH: the outer one must still hand the refreshed list on. */
    @Test
    void nestedForeachWriteReachesList() {
      database.transaction(() -> assertThat(idToV("""
          MATCH (a:Node) WHERE a.id <= 2
          WITH collect(a) AS ns
          FOREACH (x IN [1] | FOREACH (n IN ns | SET n.v = n.v + 10))
          UNWIND ns AS m
          RETURN m.id AS id, m.v AS v ORDER BY id""")).containsExactly(1L, 11L, 2L, 12L));
    }

    /** Control: FOREACH over one node with SET, read back through the node variable itself (#5114). */
    @Test
    void singleNodeForeachSetVisibleToReturn() {
      database.transaction(() -> assertThat(single("""
          MATCH (n:Node {id:2})
          FOREACH (x IN [1] | SET n.v = 7)
          RETURN n.v AS nv""", "nv")).isEqualTo(7L));

      assertThat(committedV()).containsExactly(1L, 1L, 2L, 7L, 3L, 3L);
    }

    /** Control: a FOREACH that writes nothing leaves the list as it was. */
    @Test
    void readOnlyForeachBodyLeavesListUnchanged() {
      database.transaction(() -> assertThat(idToV("""
          MATCH (a:Node) WHERE a.id <= 2
          WITH collect(a) AS ns
          FOREACH (n IN ns | CREATE (:Log {id: n.id}))
          UNWIND ns AS m
          RETURN m.id AS id, m.v AS v ORDER BY id""")).containsExactly(1L, 1L, 2L, 2L));
    }

    private long single(final String query, final String column) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        final long value = rs.next().<Number>getProperty(column).longValue();
        assertThat(rs.hasNext()).isFalse();
        return value;
      }
    }

    /** The rows of a statement returning id and v, flattened as id, v, id, v... */
    private List<Long> idToV(final String query) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        return flatten(rs);
      }
    }

    /** What an independent query reads after the commit. */
    private List<Long> committedV() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Node) RETURN n.id AS id, n.v AS v ORDER BY id")) {
        return flatten(rs);
      }
    }

    private List<Long> flatten(final ResultSet rs) {
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.<Number>getProperty("id").longValue());
        values.add(row.<Number>getProperty("v").longValue());
      }
      return values;
    }
  }
}
